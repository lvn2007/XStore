#ifndef XSTORE_XSTORE_INTERNAL_H
#define XSTORE_XSTORE_INTERNAL_H

#include "include/XStore/XStore.h"
#include "include/DataStructs/FileRangeLock.h"
#include <cstring>
#include <string_view>
#include <unistd.h>
#include <vector>

// Helpers shared by the XStore_*.cpp translation units. Not part of the
// public XStore.h surface -- callers outside src/XStore/ have no reason to
// see these.

// Writes tag+body directly into *status (a protobuf string field's own
// storage, via mutable_*()/add_*()) instead of building a separate "[TAG] "
// + body temporary and letting set_*() copy that in -- one allocation
// (sized exactly via reserve) instead of two, no intermediate std::string.
// Returns status so callers needing to append more (see RemoveDBFiles'
// filename-append site) can chain off it.
std::string* SetTaggedStatus(std::string *status, const char *tag, const std::string &body);

// Uppercases src into a thread_local reused buffer and returns a reference
// to it. Backs every public XStore method's dbName handling: those take
// dbName as a std::string_view parameter (a trivial pointer+length pass,
// not a std::string by-value construction/copy at the call boundary) and
// call this once to get the mutable, uppercase copy CachedDbBasePath/
// GetOrLoadCfg/etc. key their lookups on. One pass over the bytes (read,
// uppercase, write) instead of the old by-value-copy-then-transform-in-
// place's two passes. Safe as a single shared buffer, not one per call
// site: every XStore public method is a straight-line leaf call -- none of
// them call another XStore public method that would also need this buffer
// before the first one returns.
inline const std::string& UpperCaseInto(std::string_view src) {
    thread_local std::string Buf;
    Buf.resize(src.size());
    for (size_t i = 0; i < src.size(); i++) {
        char c = src[i];
        // Only mask the case bit off actual lowercase letters -- see
        // ToUpperCase's comment in XStore.h for why unconditional masking
        // corrupts other byte ranges.
        Buf[i] = (c >= 'a' && c <= 'z') ? static_cast<char>(c & ~0x20) : c;
    }
    return Buf;
}

// Shared by BatchInsert/BatchQuery/BatchDelete/RangeInsert: buckets a
// request's row indices by year-file. A single request realistically spans
// only a handful of distinct years, so a flat vector found via linear scan
// beats both std::map (per-key tree-node allocation, O(log K) lookup) and
// std::unordered_map (hashing overhead, O(n) worst case) for that small K --
// no pointer-chasing, the whole bucket list fits in a couple cache lines.
// Iteration order is first-seen-year order unless the caller explicitly
// sorts afterward (BatchQuery does, to keep its response chronological
// across years the way the old std::map-based version did for free).
using YearBucketMap = std::vector<std::pair<unsigned short int, std::vector<unsigned long long int>>>;

inline void AppendToYearBucket(YearBucketMap &buckets, unsigned short int year, unsigned long long int idx) {
    for (auto &Bucket : buckets) {
        if (Bucket.first == year) {
            Bucket.second.push_back(idx);
            return;
        }
    }
    buckets.emplace_back(year, std::vector<unsigned long long int>{idx});
}

// Shared by XStore_BatchOps.cpp's BatchInsert and XStore_RangeOps.cpp's
// RangeInsert: writes one year-group's rows into fd under a single lock
// spanning the group's min-to-max offset (instead of one lock per row --
// see the comment on that lock at each call site; that granularity was
// tried at both extremes -- per-row and this whole-span -- and whole-span
// won, so it's left alone here).
//
// The *write* side is coalesced independently of the lock: rows are sorted
// into maximal contiguous runs (adjacent offsets, no gap) and each run is
// serialized into one buffer and issued as a single pwrite() spanning the
// whole run, instead of one pwrite() per row. A fully sequential group
// collapses to one run -- one pwrite() for the entire group, mirroring the
// syscall-coalescing an fwrite()-style buffer would give for free. A fully
// scattered (RAND) group degrades to one run per row -- same syscall count
// as before, never worse. GetEntry(idx) must return a message with
// .timestamp()/.data() -- BatchInsert_DATA and RangeInsert_DATA both
// qualify, so this is templated on the accessor rather than the message
// type to avoid materializing an intermediate row vector per call.
// Precondition (same as RangeInsert's existing one): indices is ordered so
// that indices.front()/back() carry the group's earliest/latest timestamp.
// Templated on EntryAccessor, a distinct lambda type at each of the two call
// sites, so the definition has to stay visible here (a header) rather than
// living in a single .cpp -- both callers instantiate it with their own
// accessor lambda.
template <typename EntryAccessor>
void WriteRowGroupToFile(const std::string &targetFile, int fd, long firstEpoch,
                          unsigned int rowLength, const std::vector<unsigned long long int> &indices,
                          EntryAccessor &&getEntry) {
    const unsigned long long int FirstOffset = (static_cast<unsigned long long>(getEntry(indices.front()).timestamp()) - firstEpoch) * rowLength;
    const unsigned long long int LastOffset = (static_cast<unsigned long long>(getEntry(indices.back()).timestamp()) - firstEpoch) * rowLength;
    const unsigned long long int RegionLength = (LastOffset + rowLength) - FirstOffset;

    FileRangeLockGuard Guard(targetFile, FirstOffset, RegionLength, FileRangeLock::LockType::WRITE);

    struct ValidRow {
        unsigned long long int Idx;
        unsigned long long int Offset;
        size_t SerializedSize;
    };

    // Rows that don't fit at all are rejected rather than silently truncated
    // (matches UnaryInsert's check) -- left out of Valid entirely (not
    // zeroed, not written) so a rejected row breaks run-detection below the
    // same way any other offset gap does, rather than getting a written
    // slot of zeros it was never supposed to have. ByteSizeLong() walks
    // every field, so it's computed once here and carried through instead
    // of being recomputed in the write pass.
    std::vector<ValidRow> Valid;
    Valid.reserve(indices.size());
    for (unsigned long long int Idx : indices) {
        const auto &Entry = getEntry(Idx);
        size_t SerializedSize = Entry.data().ByteSizeLong();
        if (SerializedSize <= static_cast<size_t>(rowLength)) {
            unsigned long long int Offset = (static_cast<unsigned long long>(Entry.timestamp()) - firstEpoch) * rowLength;
            Valid.push_back({Idx, Offset, SerializedSize});
        }
    }

    // Thread-local scratch buffer, grown to fit the largest run written so
    // far and reused across calls -- avoids a heap allocation per run.
    thread_local std::vector<char> RunBuf;

    for (size_t RunStart = 0; RunStart < Valid.size(); ) {
        size_t RunEnd = RunStart + 1;
        while (RunEnd < Valid.size() && Valid[RunEnd].Offset == Valid[RunEnd - 1].Offset + rowLength) {
            ++RunEnd;
        }

        size_t RunBytes = (RunEnd - RunStart) * static_cast<size_t>(rowLength);
        if (RunBuf.size() < RunBytes) {
            RunBuf.resize(RunBytes);
        }

        for (size_t i = RunStart; i < RunEnd; ++i) {
            char *Dest = RunBuf.data() + (i - RunStart) * rowLength;
            const XStore_Proto::RowData &Data = getEntry(Valid[i].Idx).data();

            // Serialize first, then zero only the tail past the actual
            // serialized size -- no stale trailing bytes from a previous
            // row leak through (same guarantee as UnaryInsert), without
            // redundantly zeroing bytes SerializeToArray is about to
            // overwrite anyway.
            Data.SerializeToArray(Dest, rowLength);
            std::memset(Dest + Valid[i].SerializedSize, 0, rowLength - Valid[i].SerializedSize);
        }

        pwrite(fd, RunBuf.data(), RunBytes, static_cast<off_t>(Valid[RunStart].Offset));
        RunStart = RunEnd;
    }
}

#endif //XSTORE_XSTORE_INTERNAL_H
