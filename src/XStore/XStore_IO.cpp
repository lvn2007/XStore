#include "include/XStore/XStore.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

std::list<std::pair<std::string, XStore::MappedFile>> XStore::MmapLru;
std::unordered_map<std::string, std::list<std::pair<std::string, XStore::MappedFile>>::iterator> XStore::MmapIndex;
std::mutex XStore::MmapMutex;

std::list<std::pair<std::string, int>> XStore::FdLru;
std::unordered_map<std::string, std::list<std::pair<std::string, int>>::iterator> XStore::FdIndex;
std::mutex XStore::FdMutex;

void* XStore::GetOrMapFile(const std::string &targetFile, size_t rowLength, unsigned long long totalRows) {
    // Fast path: already mapped -- only the lookup+MRU-promote touches
    // shared state, so the lock is held just long enough for that.
    {
        std::lock_guard<std::mutex> Lock(MmapMutex);
        auto it = MmapIndex.find(targetFile);
        if (it != MmapIndex.end()) {
            MmapLru.splice(MmapLru.begin(), MmapLru, it->second);
            return it->second->second.Base;
        }
    }

    // Cache miss -> open (create if needed), ensure size, map. Done WITHOUT
    // the lock: open/fstat/ftruncate/mmap are syscalls whose cost scales
    // with file size and disk state, and holding a process-wide mutex
    // across them would serialize every other thread's inserts -- to any
    // DB, not just this one -- for that syscall's whole duration. Two
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
    std::lock_guard<std::mutex> Lock(MmapMutex);
    auto it = MmapIndex.find(targetFile);
    if (it != MmapIndex.end()) {
        // Lost the race -- drop our redundant mapping, use theirs.
        munmap(Base, FileLen);
        MmapLru.splice(MmapLru.begin(), MmapLru, it->second);
        return it->second->second.Base;
    }

    MmapLru.emplace_front(targetFile, MappedFile{Base, FileLen});
    MmapIndex[targetFile] = MmapLru.begin();

    if (MmapLru.size() > MmapCacheCap) {
        munmap(MmapLru.back().second.Base, MmapLru.back().second.Len);
        MmapIndex.erase(MmapLru.back().first);
        MmapLru.pop_back();
    }

    return Base;
}

int XStore::GetOrOpenFile(const std::string &targetFile, size_t rowLength, unsigned long long totalRows) {
    // Fast path: already open -- only the lookup+MRU-promote touches shared
    // state, mirrors GetOrMapFile's fast path.
    {
        std::lock_guard<std::mutex> Lock(FdMutex);
        auto it = FdIndex.find(targetFile);
        if (it != FdIndex.end()) {
            FdLru.splice(FdLru.begin(), FdLru, it->second);
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
    std::lock_guard<std::mutex> Lock(FdMutex);
    auto it = FdIndex.find(targetFile);
    if (it != FdIndex.end()) {
        // Lost the race -- drop our redundant fd, use theirs.
        close(Fd);
        FdLru.splice(FdLru.begin(), FdLru, it->second);
        return it->second->second;
    }

    FdLru.emplace_front(targetFile, Fd);
    FdIndex[targetFile] = FdLru.begin();

    if (FdLru.size() > FdCacheCap) {
        close(FdLru.back().second);
        FdIndex.erase(FdLru.back().first);
        FdLru.pop_back();
    }

    return Fd;
}

void XStore::EraseOpenFile(const std::string &targetFile) {
    std::lock_guard<std::mutex> Lock(FdMutex);

    auto It = FdIndex.find(targetFile);
    if (It == FdIndex.end()) {
        return;
    }

    close(It->second->second);
    FdLru.erase(It->second);
    FdIndex.erase(It);
}

void XStore::EraseOpenFilesUnderPrefix(const std::string &dbDirPrefix) {
    std::lock_guard<std::mutex> Lock(FdMutex);

    for (auto It = FdLru.begin(); It != FdLru.end();) {
        if (It->first.compare(0, dbDirPrefix.size(), dbDirPrefix) == 0) {
            close(It->second);
            FdIndex.erase(It->first);
            It = FdLru.erase(It);
        } else {
            ++It;
        }
    }
}

void XStore::EraseMappedFile(const std::string &targetFile) {
    std::lock_guard<std::mutex> Lock(MmapMutex);

    auto It = MmapIndex.find(targetFile);
    if (It == MmapIndex.end()) {
        return;
    }

    munmap(It->second->second.Base, It->second->second.Len);
    MmapLru.erase(It->second);
    MmapIndex.erase(It);
}

void XStore::EraseMappedFilesUnderPrefix(const std::string &dbDirPrefix) {
    std::lock_guard<std::mutex> Lock(MmapMutex);

    for (auto It = MmapLru.begin(); It != MmapLru.end();) {
        if (It->first.compare(0, dbDirPrefix.size(), dbDirPrefix) == 0) {
            munmap(It->second.Base, It->second.Len);
            MmapIndex.erase(It->first);
            It = MmapLru.erase(It);
        } else {
            ++It;
        }
    }
}

// Shared mmap-backed scan kernels used by MinAggregate/MaxAggregate/SumAggregate/AvgAggregate.
//  One page-aligned mmap per file range, MADV_SEQUENTIAL readahead, zero per-row syscalls,
//  CodedInputStream parses directly off mapped pages (no row buffer copy).

template <typename Cmp>
void XStore::ProcessFileRangeSelect(FILE* bFile, unsigned long long startOffset, unsigned long long totalRows,
                                     size_t rowLen, unsigned short columnIdx,
                                     ValueStruct& bestValue, XStore_Proto::FData& bestData) {

    if (totalRows == 0) return;

    int Fd = fileno(bFile);
    long PageSize = sysconf(_SC_PAGE_SIZE);

    unsigned long long AlignedOffset = (startOffset / PageSize) * PageSize;
    unsigned long long AlignmentSlop = startOffset - AlignedOffset;
    size_t TotalBytesToMap = (totalRows * rowLen) + AlignmentSlop;

    void* MmappedPtr = mmap(NULL, TotalBytesToMap, PROT_READ, MAP_SHARED, Fd, AlignedOffset);
    if (MmappedPtr == MAP_FAILED) return;

    madvise(MmappedPtr, TotalBytesToMap, MADV_SEQUENTIAL | MADV_WILLNEED);

    const char* BasePtr = static_cast<const char*>(MmappedPtr) + AlignmentSlop;
    XStore_Proto::RowData TempData;
    ValueStruct TempValue;
    const Cmp Better{}; // stateless functor, no per-call construction cost

    for (unsigned long long i = 0; i < totalRows; ++i) {
        const char* RowPtr = BasePtr + (i * rowLen);

        // Instant skip for empty slots
        if (RowPtr[0] == 0) continue;

        TempData.Clear();

        // We MUST use CodedInputStream to properly ignore trailing zeros in the fixed-width row slot
        google::protobuf::io::ArrayInputStream ArrayIn(RowPtr, (int)rowLen);
        google::protobuf::io::CodedInputStream CodedIn(&ArrayIn);

        if (!TempData.MergePartialFromCodedStream(&CodedIn)) {
            continue;
        }

        if (columnIdx < (unsigned int)TempData.fielddata_size()) {
            const auto& CurrData = TempData.fielddata(columnIdx);

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

    munmap(MmappedPtr, TotalBytesToMap);
}

// Explicit instantiation: these are the only two comparators ever used, so the compiler
// generates two fully-inlined, branch-free (on Cmp) versions -- no vtable/std::function overhead.
template void XStore::ProcessFileRangeSelect<std::less<long double>>(
    FILE*, unsigned long long, unsigned long long, size_t, unsigned short, ValueStruct&, XStore_Proto::FData&);
template void XStore::ProcessFileRangeSelect<std::greater<long double>>(
    FILE*, unsigned long long, unsigned long long, size_t, unsigned short, ValueStruct&, XStore_Proto::FData&);

void XStore::ProcessFileRangeAccumulate(FILE* bFile, unsigned long long startOffset, unsigned long long totalRows,
                                         size_t rowLen, unsigned short columnIdx,
                                         double& sumOut, unsigned long long& validCountOut) {

    if (totalRows == 0) return;

    int Fd = fileno(bFile);
    long PageSize = sysconf(_SC_PAGE_SIZE);

    unsigned long long AlignedOffset = (startOffset / PageSize) * PageSize;
    unsigned long long AlignmentSlop = startOffset - AlignedOffset;
    size_t TotalBytesToMap = (totalRows * rowLen) + AlignmentSlop;

    void* MmappedPtr = mmap(NULL, TotalBytesToMap, PROT_READ, MAP_SHARED, Fd, AlignedOffset);
    if (MmappedPtr == MAP_FAILED) return;

    madvise(MmappedPtr, TotalBytesToMap, MADV_SEQUENTIAL | MADV_WILLNEED);

    const char* BasePtr = static_cast<const char*>(MmappedPtr) + AlignmentSlop;
    XStore_Proto::RowData TempData;
    ValueStruct TempValue;

    for (unsigned long long i = 0; i < totalRows; ++i) {
        const char* RowPtr = BasePtr + (i * rowLen);

        if (RowPtr[0] == 0) continue;

        TempData.Clear();

        google::protobuf::io::ArrayInputStream ArrayIn(RowPtr, (int)rowLen);
        google::protobuf::io::CodedInputStream CodedIn(&ArrayIn);

        if (!TempData.MergePartialFromCodedStream(&CodedIn)) {
            continue;
        }

        if (columnIdx < (unsigned int)TempData.fielddata_size()) {
            const auto& CurrData = TempData.fielddata(columnIdx);

            if (CurrData.oneOfData_case() > 0 && CurrData.oneOfData_case() <= 12) {
                XStore::GetFDataValue(&CurrData, &TempValue);
                sumOut += (double)TempValue.Value;
                validCountOut++;
            }
        }
    }

    munmap(MmappedPtr, TotalBytesToMap);
}

// Profiling-only prototype -- see header comment on the declaration and
// tests/perf/XStorePerfTest.cpp for how it's benchmarked against MinAggregate.
template <typename Cmp>
void XStore::ProcessFileRangeSelectTypedDouble(FILE* bFile, unsigned long long startOffset, unsigned long long totalRows,
                                     size_t rowLen, unsigned short columnIdx,
                                     bool& bestInitialized, double& bestValue, XStore_Proto::FData& bestData) {

    if (totalRows == 0) return;

    int Fd = fileno(bFile);
    long PageSize = sysconf(_SC_PAGE_SIZE);

    unsigned long long AlignedOffset = (startOffset / PageSize) * PageSize;
    unsigned long long AlignmentSlop = startOffset - AlignedOffset;
    size_t TotalBytesToMap = (totalRows * rowLen) + AlignmentSlop;

    void* MmappedPtr = mmap(NULL, TotalBytesToMap, PROT_READ, MAP_SHARED, Fd, AlignedOffset);
    if (MmappedPtr == MAP_FAILED) return;

    madvise(MmappedPtr, TotalBytesToMap, MADV_SEQUENTIAL | MADV_WILLNEED);

    const char* BasePtr = static_cast<const char*>(MmappedPtr) + AlignmentSlop;
    XStore_Proto::RowData TempData;
    const Cmp Better{};

    for (unsigned long long i = 0; i < totalRows; ++i) {
        const char* RowPtr = BasePtr + (i * rowLen);

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

    munmap(MmappedPtr, TotalBytesToMap);
}

template void XStore::ProcessFileRangeSelectTypedDouble<std::less<double>>(
    FILE*, unsigned long long, unsigned long long, size_t, unsigned short, bool&, double&, XStore_Proto::FData&);

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
