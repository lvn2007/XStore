#ifndef XSTORE_XSTORE_INTERNAL_H
#define XSTORE_XSTORE_INTERNAL_H

#include "include/XStore/XStore.h"
#include <algorithm>
#include <execution>
#include <iterator>
#include <string_view>
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

// Insert entry-point guard: after XStore::Shutdown() the WAL writers are
// gone (a SYNC enqueue would block forever), so every insert path rejects
// up front. Writes an [ERROR] status and returns true when shut down.
inline bool RejectInsertIfShutDown(std::string *status, const char *op) {
    if (!XStore::IsShutDown()) return false;
    SetTaggedStatus(status, "[ERROR] ", std::string("[") + op + "] - Server has shut down; insert rejected");
    return true;
}

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

// std::execution::par_unseq dispatches work across a thread pool -- worth it
// once a batch is big enough that sorting it in parallel outweighs that
// dispatch/join overhead, but for a small batch (the common case for most
// callers) the overhead alone can cost more than a plain sequential sort
// would take to run the whole thing. Used by Batch Insert/Query/Delete's
// "sort incoming timestamps first" step. NOT stable (equal timestamps may swap);
// RangeInsert's sort-on-inversion fallback uses std::stable_sort instead.
inline constexpr std::ptrdiff_t PARALLEL_SORT_THRESHOLD = 10'000;

template <typename Iterator, typename Compare>
void SortBatch(Iterator begin, Iterator end, Compare comp) {
    if (std::distance(begin, end) >= PARALLEL_SORT_THRESHOLD) {
        std::sort(std::execution::par_unseq, begin, end, comp);
    } else {
        std::sort(begin, end, comp);
    }
}

// Definition of XStore::BuildZoneMapObservations (declared in XStore.h,
// private static). Built once at WAL enqueue time (row_bytes --
// already-serialized wire bytes -- is all ApplyWalRecord has later, not the
// original RowData, so this can't be deferred to apply time). Only emits
// observations for columns actually present in the row (col <
// row.fielddata_size()) -- columns past that, up to tracked_column_count,
// are exactly as much a hole as an explicitly unset field at a present
// index (XStoreZoneMapTest.UnaryInsertTreatsHoleAsNullNotZero -- an entirely
// empty row still records a hole at column 0), but ZoneMapFile::UpdateRow's
// pad_to parameter now bumps null_count for those instead of this building
// an explicit is_numeric=false entry per hole -- same total effect, no
// wasted observations for a narrow row against a wide tracked width.
// tracked_column_count comes from XStore::EffectiveTrackedColumnCount(cfg_struct)
// at every call site -- the per-DB columnCount high-water mark when it's
// been seeded/grown past 0, otherwise the global ZONE_MAP_MAX_TRACKED_COLUMNS
// default. Numeric-case check mirrors ProcessFileRangeSelect/
// ProcessFileRangeAccumulate's own oneOfData_case() guard (XStore_IO.cpp) --
// same "1..12 is numeric" rule the aggregate kernels already use, so a
// bool/string/bytes column is a hole here too, not a crash or a silently-wrong 0.
inline ZoneMapObservation XStore::ZoneMapObservationOf(XStore &store, const XStore_Proto::FData &f_data, uint32_t column_idx) {
    bool is_numeric = f_data.oneOfData_case() > 0 && f_data.oneOfData_case() <= 12;
    ValueStruct vs{};
    if (is_numeric) {
        store.GetFDataValue(&f_data, &vs);
    }
    return {column_idx, static_cast<double>(vs.value), is_numeric};
}

inline std::vector<ZoneMapObservation> XStore::BuildZoneMapObservations(XStore &store, const XStore_Proto::RowData &row, uint32_t tracked_column_count) {
    const int tracked = std::min(static_cast<int>(tracked_column_count), row.fielddata_size());
    std::vector<ZoneMapObservation> observations;
    observations.reserve(static_cast<size_t>(std::max(tracked, 0)));
    for (int col = 0; col < tracked; ++col) {
        observations.push_back(ZoneMapObservationOf(store, row.fielddata(col), static_cast<uint32_t>(col)));
    }
    return observations;
}

// Definition of XStore::BuildCatIdxObservations (declared in XStore.h,
// private static). Same enqueue-time-build contract as
// BuildZoneMapObservations above, but covers ALL FData oneof cases -- not
// gated to numeric -- since CatIdx's bloom filter works on
// EncodeFDataForCatIdx's encoded byte string regardless of the field's
// type (int/double/string/bytes/bool all encode to something hashable).
// Only emits an entry for columns that are both present AND set --
// CatIdxFile::UpdateRow already skips !is_present entries, so there is no
// pad_to equivalent needed here: omitting the entry outright has the exact
// same effect as pushing one with is_present=false.
inline std::vector<CatIdxObservation> XStore::BuildCatIdxObservations(XStore &store, const XStore_Proto::RowData &row, uint32_t tracked_column_count) {
    (void)store; // unused -- unlike BuildZoneMapObservations, no GetFDataValue coercion needed; EncodeFDataForCatIdx reads FData directly
    const int tracked = std::min(static_cast<int>(tracked_column_count), row.fielddata_size());
    std::vector<CatIdxObservation> observations;
    observations.reserve(static_cast<size_t>(std::max(tracked, 0)));
    for (int col = 0; col < tracked; ++col) {
        if (row.fielddata(col).oneOfData_case() != XStore_Proto::FData::ONEOFDATA_NOT_SET) {
            observations.push_back({static_cast<uint32_t>(col), EncodeFDataForCatIdx(row.fielddata(col)), true});
        }
    }
    return observations;
}

#endif //XSTORE_XSTORE_INTERNAL_H
