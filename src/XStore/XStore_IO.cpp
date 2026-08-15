#include "include/XStore/XStore.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/wire_format_lite.h>

std::array<XStore::MmapShard, XStore::MmapShardCount> XStore::MmapShards;
std::array<XStore::FdShard, XStore::FdShardCount> XStore::FdShards;

void* XStore::GetOrMapFile(const std::string &targetFile, size_t rowLength, unsigned long long totalRows) {
    MmapShard &Shard = MmapShards[std::hash<std::string>{}(targetFile) % MmapShardCount];

    // Fast path: already mapped -- only the lookup+MRU-promote touches this
    // shard's state, so the lock is held just long enough for that, and
    // only contends with other threads hashing to the same shard.
    {
        std::lock_guard<std::mutex> Lock(Shard.Mutex);
        auto it = Shard.Index.find(targetFile);
        if (it != Shard.Index.end()) {
            Shard.Lru.splice(Shard.Lru.begin(), Shard.Lru, it->second);
            return it->second->second.Base;
        }
    }

    // Cache miss -> open (create if needed), ensure size, map. Done WITHOUT
    // the lock: open/fstat/ftruncate/mmap are syscalls whose cost scales
    // with file size and disk state, and holding this shard's mutex across
    // them would serialize every other thread hashing to the same shard --
    // to any DB, not just this one -- for that syscall's whole duration. Two
    // threads racing to first-touch the same file both pay this cost
    // unlocked; the loser's redundant mapping is dropped below.
    int Fd = open(targetFile.c_str(), O_RDWR | O_CREAT, 0644);
    if (Fd < 0) {
        return nullptr;
    }

    // +1 row: totalRows is (LastEpoch - FirstEpoch), but a write at
    // unixTime == LastEpoch (the year's final second, inclusive) lands at
    // offset totalRows*rowLength -- one row past that span. The original
    // fwrite()-based writer silently grew the file to cover this via
    // EOF-extension; a fixed-size mapping needs the extra row up front.
    size_t FileLen = (static_cast<size_t>(totalRows) + 1) * rowLength;

    struct stat St{};
    if (fstat(Fd, &St) != 0 || static_cast<size_t>(St.st_size) < FileLen) {
        if (ftruncate(Fd, FileLen) != 0) {
            close(Fd);
            return nullptr;
        }
    }

    void *Base = mmap(NULL, FileLen, PROT_READ | PROT_WRITE, MAP_SHARED, Fd, 0);
    // Mapping stays valid after the fd is closed -- no need to keep it open.
    close(Fd);
    if (Base == MAP_FAILED) {
        return nullptr;
    }

    // Re-acquire to install into the cache, double-checked: another thread
    // may have raced us to map this same file while we were unlocked.
    std::lock_guard<std::mutex> Lock(Shard.Mutex);
    auto it = Shard.Index.find(targetFile);
    if (it != Shard.Index.end()) {
        // Lost the race -- drop our redundant mapping, use theirs.
        munmap(Base, FileLen);
        Shard.Lru.splice(Shard.Lru.begin(), Shard.Lru, it->second);
        return it->second->second.Base;
    }

    Shard.Lru.emplace_front(targetFile, MappedFile{Base, FileLen});
    Shard.Index[targetFile] = Shard.Lru.begin();

    if (Shard.Lru.size() > MmapShardCap) {
        munmap(Shard.Lru.back().second.Base, Shard.Lru.back().second.Len);
        Shard.Index.erase(Shard.Lru.back().first);
        Shard.Lru.pop_back();
    }

    return Base;
}

int XStore::GetOrOpenFile(const std::string &targetFile, size_t rowLength, unsigned long long totalRows) {
    FdShard &Shard = FdShards[std::hash<std::string>{}(targetFile) % FdShardCount];

    // Fast path: already open -- only the lookup+MRU-promote touches this
    // shard's state, mirrors GetOrMapFile's fast path.
    {
        std::lock_guard<std::mutex> Lock(Shard.Mutex);
        auto it = Shard.Index.find(targetFile);
        if (it != Shard.Index.end()) {
            Shard.Lru.splice(Shard.Lru.begin(), Shard.Lru, it->second);
            return it->second->second;
        }
    }

    // Cache miss -> open (create if needed), ensure size. Done WITHOUT the
    // lock for the same reason as GetOrMapFile: these are syscalls whose
    // cost shouldn't serialize every other thread's inserts.
    int Fd = open(targetFile.c_str(), O_RDWR | O_CREAT, 0644);
    if (Fd < 0) {
        return -1;
    }

    // +1 row: see GetOrMapFile for why.
    size_t FileLen = (static_cast<size_t>(totalRows) + 1) * rowLength;

    struct stat St{};
    if (fstat(Fd, &St) != 0 || static_cast<size_t>(St.st_size) < FileLen) {
        if (ftruncate(Fd, FileLen) != 0) {
            close(Fd);
            return -1;
        }
    }

    // Re-acquire to install into the cache, double-checked: another thread
    // may have raced us to open this same file while we were unlocked.
    std::lock_guard<std::mutex> Lock(Shard.Mutex);
    auto it = Shard.Index.find(targetFile);
    if (it != Shard.Index.end()) {
        // Lost the race -- drop our redundant fd, use theirs.
        close(Fd);
        Shard.Lru.splice(Shard.Lru.begin(), Shard.Lru, it->second);
        return it->second->second;
    }

    Shard.Lru.emplace_front(targetFile, Fd);
    Shard.Index[targetFile] = Shard.Lru.begin();

    if (Shard.Lru.size() > FdShardCap) {
        close(Shard.Lru.back().second);
        Shard.Index.erase(Shard.Lru.back().first);
        Shard.Lru.pop_back();
    }

    return Fd;
}

bool XStore::IsFdCached(const std::string &targetFile) {
    FdShard &Shard = FdShards[std::hash<std::string>{}(targetFile) % FdShardCount];
    std::lock_guard<std::mutex> Lock(Shard.Mutex);
    return Shard.Index.find(targetFile) != Shard.Index.end();
}

void XStore::EraseOpenFile(const std::string &targetFile) {
    FdShard &Shard = FdShards[std::hash<std::string>{}(targetFile) % FdShardCount];
    std::lock_guard<std::mutex> Lock(Shard.Mutex);

    auto It = Shard.Index.find(targetFile);
    if (It == Shard.Index.end()) {
        return;
    }

    close(It->second->second);
    Shard.Lru.erase(It->second);
    Shard.Index.erase(It);
}

void XStore::EraseOpenFilesUnderPrefix(const std::string &dbDirPrefix) {
    // Same rationale as EraseMappedFilesUnderPrefix -- sweep each shard
    // under its own lock rather than one lock for the whole sweep.
    for (FdShard &Shard : FdShards) {
        std::lock_guard<std::mutex> Lock(Shard.Mutex);

        for (auto It = Shard.Lru.begin(); It != Shard.Lru.end();) {
            if (It->first.compare(0, dbDirPrefix.size(), dbDirPrefix) == 0) {
                close(It->second);
                Shard.Index.erase(It->first);
                It = Shard.Lru.erase(It);
            } else {
                ++It;
            }
        }
    }
}

void XStore::EraseMappedFile(const std::string &targetFile) {
    MmapShard &Shard = MmapShards[std::hash<std::string>{}(targetFile) % MmapShardCount];
    std::lock_guard<std::mutex> Lock(Shard.Mutex);

    auto It = Shard.Index.find(targetFile);
    if (It == Shard.Index.end()) {
        return;
    }

    munmap(It->second->second.Base, It->second->second.Len);
    Shard.Lru.erase(It->second);
    Shard.Index.erase(It);
}

void XStore::EraseMappedFilesUnderPrefix(const std::string &dbDirPrefix) {
    // Prefix sweep doesn't know which shard(s) hold matching files, so it
    // walks all of them -- locked one shard at a time (not all at once),
    // so concurrent GetOrMapFile() calls hashing to other shards aren't
    // blocked for the sweep's duration. Rare/administrative path (DB
    // removal), not a hot path, so this is fine.
    for (MmapShard &Shard : MmapShards) {
        std::lock_guard<std::mutex> Lock(Shard.Mutex);

        for (auto It = Shard.Lru.begin(); It != Shard.Lru.end();) {
            if (It->first.compare(0, dbDirPrefix.size(), dbDirPrefix) == 0) {
                munmap(It->second.Base, It->second.Len);
                Shard.Index.erase(It->first);
                It = Shard.Lru.erase(It);
            } else {
                ++It;
            }
        }
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

template <typename Cmp>
void XStore::ProcessFileRangeSelect(const char* rowsBase, unsigned long long totalRows,
                                     size_t rowLen, unsigned short columnIdx,
                                     ValueStruct& bestValue, XStore_Proto::FData& bestData) {

    if (totalRows == 0) return;

    XStore_Proto::FData CurrData;
    ValueStruct TempValue;
    const Cmp Better{}; // stateless functor, no per-call construction cost

    for (unsigned long long i = 0; i < totalRows; ++i) {
        const char* RowPtr = rowsBase + (i * rowLen);

        // Instant skip for empty slots
        if (RowPtr[0] == 0) continue;

        CurrData.Clear();
        if (!ExtractColumnByIndexSkipParse(RowPtr, rowLen, columnIdx, CurrData)) continue;

        if (CurrData.oneOfData_case() > 0 && CurrData.oneOfData_case() <= 12) {
            XStore::GetFDataValue(&CurrData, &TempValue);

            if (!bestValue.IsInitialized || Better(TempValue.Value, bestValue.Value)) {
                bestValue.Value = TempValue.Value;
                bestValue.IsInitialized = true;
                bestData = CurrData;
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

void XStore::ProcessFileRangeAccumulate(const char* rowsBase, unsigned long long totalRows,
                                         size_t rowLen, unsigned short columnIdx,
                                         double& sumOut, unsigned long long& validCountOut) {

    if (totalRows == 0) return;

    XStore_Proto::FData CurrData;
    ValueStruct TempValue;

    for (unsigned long long i = 0; i < totalRows; ++i) {
        const char* RowPtr = rowsBase + (i * rowLen);

        if (RowPtr[0] == 0) continue;

        CurrData.Clear();
        if (!ExtractColumnByIndexSkipParse(RowPtr, rowLen, columnIdx, CurrData)) continue;

        if (CurrData.oneOfData_case() > 0 && CurrData.oneOfData_case() <= 12) {
            XStore::GetFDataValue(&CurrData, &TempValue);
            sumOut += (double)TempValue.Value;
            validCountOut++;
        }
    }
}

// Profiling-only prototype -- see header comment on the declaration and
// tests/perf/XStorePerfTest.cpp for how it's benchmarked against MinAggregate.
template <typename Cmp>
void XStore::ProcessFileRangeSelectTypedDouble(const char* rowsBase, unsigned long long totalRows,
                                     size_t rowLen, unsigned short columnIdx,
                                     bool& bestInitialized, double& bestValue, XStore_Proto::FData& bestData) {

    if (totalRows == 0) return;

    XStore_Proto::RowData TempData;
    const Cmp Better{};

    for (unsigned long long i = 0; i < totalRows; ++i) {
        const char* RowPtr = rowsBase + (i * rowLen);

        if (RowPtr[0] == 0) continue;

        TempData.Clear();

        google::protobuf::io::ArrayInputStream ArrayIn(RowPtr, (int)rowLen);
        google::protobuf::io::CodedInputStream CodedIn(&ArrayIn);

        if (!TempData.MergePartialFromCodedStream(&CodedIn)) {
            continue;
        }

        if (columnIdx < (unsigned int)TempData.fielddata_size()) {
            const auto& CurrData = TempData.fielddata(columnIdx);

            // Native-type compare: no GetFDataValue switch, no long double
            // conversion. Real version would determine this case once (not
            // per row) from the first valid row seen.
            if (CurrData.oneOfData_case() == XStore_Proto::FData::OneOfDataCase::kDoubleData) {
                double CandidateValue = CurrData.doubledata();

                if (!bestInitialized || Better(CandidateValue, bestValue)) {
                    bestValue = CandidateValue;
                    bestInitialized = true;
                    bestData = CurrData;
                }
            }
        }
    }
}

template void XStore::ProcessFileRangeSelectTypedDouble<std::less<double>>(
    const char*, unsigned long long, size_t, unsigned short, bool&, double&, XStore_Proto::FData&);

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
bool XStore::ExtractColumnByIndexSkipParse(const char* rowPtr, size_t rowLen,
                                            unsigned short columnIdx,
                                            XStore_Proto::FData& outData) {
    using google::protobuf::internal::WireFormatLite;

    google::protobuf::io::ArrayInputStream ArrayIn(rowPtr, (int)rowLen);
    google::protobuf::io::CodedInputStream CodedIn(&ArrayIn);

    unsigned int Occurrence = 0;
    uint32_t Tag;
    // ReadTag() returns 0 once it hits the fixed-width slot's zero padding
    // (no valid message ever encodes field number 0) -- same end-of-row
    // signal the old full-parse path relied on.
    while ((Tag = CodedIn.ReadTag()) != 0) {
        if (WireFormatLite::GetTagFieldNumber(Tag) != XStore_Proto::RowData::kFieldDataFieldNumber) {
            // RowData only ever encodes field 1 -- skip rather than assume
            // this can't happen.
            if (!WireFormatLite::SkipField(&CodedIn, Tag)) return false;
            continue;
        }

        if (Occurrence == columnIdx) {
            return WireFormatLite::ReadMessage(&CodedIn, &outData);
        }

        if (!WireFormatLite::SkipField(&CodedIn, Tag)) return false;
        ++Occurrence;
    }

    return false;  // ran out of columns before reaching columnIdx
}

void XStore::GetFDataValue(const XStore_Proto::FData *fDataInput, unsigned int columnIdx, ValueStruct *returnData) {
    switch (fDataInput->oneOfData_case()) {
        case (XStore_Proto::FData::OneOfDataCase::kDoubleData): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->doubledata();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFloatData): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->floatdata();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kInt32Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->int32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kInt64Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->int64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kUint32Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->uint32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kUint64Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->uint64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSint32Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->sint32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSint64Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->sint64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFixed32Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->fixed32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFixed64Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->fixed64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSfixed32Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->sfixed32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSfixed64Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->sfixed64data();
            break;
        }
    }
}

void XStore::GetFDataValue(const XStore_Proto::FData *fDataInput, ValueStruct *returnData) {
    switch (fDataInput->oneOfData_case()) {
        case (XStore_Proto::FData::OneOfDataCase::kDoubleData): {
            returnData->Value = fDataInput->doubledata();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFloatData): {
            returnData->Value = fDataInput->floatdata();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kInt32Data): {
            returnData->Value = fDataInput->int32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kInt64Data): {
            returnData->Value = fDataInput->int64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kUint32Data): {
            returnData->Value = fDataInput->uint32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kUint64Data): {
            returnData->Value = fDataInput->uint64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSint32Data): {
            returnData->Value = fDataInput->sint32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSint64Data): {
            returnData->Value = fDataInput->sint64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFixed32Data): {
            returnData->Value = fDataInput->fixed32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFixed64Data): {
            returnData->Value = fDataInput->fixed64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSfixed32Data): {
            returnData->Value = fDataInput->sfixed32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSfixed64Data): {
            returnData->Value = fDataInput->sfixed64data();
            break;
        }
    }
}
