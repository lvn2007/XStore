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
    thread_local std::string buf;
    buf.resize(src.size());
    for (size_t i = 0; i < src.size(); i++) {
        char c = src[i];
        // Only mask the case bit off actual lowercase letters -- see
        // ToUpperCase's comment in XStore.h for why unconditional masking
        // corrupts other byte ranges.
        buf[i] = (c >= 'a' && c <= 'z') ? static_cast<char>(c & ~0x20) : c;
    }
    return buf;
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
    for (auto &bucket : buckets) {
        if (bucket.first == year) {
            bucket.second.push_back(idx);
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
void WriteRowGroupToFile(const std::string &target_file, int fd, long first_epoch,
                          unsigned int row_length, const std::vector<unsigned long long int> &indices,
                          EntryAccessor &&get_entry) {
    const unsigned long long int first_offset = (static_cast<unsigned long long>(get_entry(indices.front()).timestamp()) - first_epoch) * row_length;
    const unsigned long long int last_offset = (static_cast<unsigned long long>(get_entry(indices.back()).timestamp()) - first_epoch) * row_length;
    const unsigned long long int region_length = (last_offset + row_length) - first_offset;

    FileRangeLockGuard guard(target_file, first_offset, region_length, FileRangeLock::LockType::WRITE);

    struct ValidRow {
        unsigned long long int idx;
        unsigned long long int offset;
        size_t serialized_size;
    };

    // Rows that don't fit at all are rejected rather than silently truncated
    // (matches UnaryInsert's check) -- left out of Valid entirely (not
    // zeroed, not written) so a rejected row breaks run-detection below the
    // same way any other offset gap does, rather than getting a written
    // slot of zeros it was never supposed to have. ByteSizeLong() walks
    // every field, so it's computed once here and carried through instead
    // of being recomputed in the write pass.
    std::vector<ValidRow> valid;
    valid.reserve(indices.size());
    for (unsigned long long int idx : indices) {
        const auto &entry = get_entry(idx);
        size_t serialized_size = entry.data().ByteSizeLong();
        if (serialized_size <= static_cast<size_t>(row_length)) {
            unsigned long long int offset = (static_cast<unsigned long long>(entry.timestamp()) - first_epoch) * row_length;
            valid.push_back({idx, offset, serialized_size});
        }
    }

    // Thread-local scratch buffer, grown to fit the largest run written so
    // far and reused across calls -- avoids a heap allocation per run.
    thread_local std::vector<char> run_buf;

    for (size_t run_start = 0; run_start < valid.size(); ) {
        size_t run_end = run_start + 1;
        while (run_end < valid.size() && valid[run_end].offset == valid[run_end - 1].offset + row_length) {
            ++run_end;
        }

        size_t run_bytes = (run_end - run_start) * static_cast<size_t>(row_length);
        if (run_buf.size() < run_bytes) {
            run_buf.resize(run_bytes);
        }

        for (size_t i = run_start; i < run_end; ++i) {
            char *dest = run_buf.data() + (i - run_start) * row_length;
            const XStore_Proto::RowData &data = get_entry(valid[i].idx).data();

            // Serialize first, then zero only the tail past the actual
            // serialized size -- no stale trailing bytes from a previous
            // row leak through (same guarantee as UnaryInsert), without
            // redundantly zeroing bytes SerializeToArray is about to
            // overwrite anyway.
            data.SerializeToArray(dest, row_length);
            std::memset(dest + valid[i].serialized_size, 0, row_length - valid[i].serialized_size);
        }

        pwrite(fd, run_buf.data(), run_bytes, static_cast<off_t>(valid[run_start].offset));
        run_start = run_end;
    }
}

#endif //XSTORE_XSTORE_INTERNAL_H
