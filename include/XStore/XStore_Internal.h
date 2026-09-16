#ifndef XSTORE_XSTORE_INTERNAL_H
#define XSTORE_XSTORE_INTERNAL_H

#include "include/XStore/XStore.h"
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

// Definition of XStore::BuildZoneMapObservations (declared in XStore.h,
// private static). Built once at WAL enqueue time (row_bytes --
// already-serialized wire bytes -- is all ApplyWalRecord has later, not the
// original RowData, so this can't be deferred to apply time). Always emits
// exactly tracked_column_count observations, one per tracked column, never
// fewer: a column past the row's own fielddata_size() is exactly as much a
// hole as an explicitly unset field at a present index
// (XStoreZoneMapTest.UnaryInsertTreatsHoleAsNullNotZero -- an entirely
// empty row still records a hole at column 0), so this does NOT stop at
// row.fielddata_size(). tracked_column_count comes from
// XStore::EffectiveTrackedColumnCount(cfg_struct) at every call site --
// the per-DB columnCount high-water mark when it's been seeded/grown past
// 0, otherwise the global ZONE_MAP_MAX_TRACKED_COLUMNS default. Numeric-case
// check mirrors ProcessFileRangeSelect/ProcessFileRangeAccumulate's own
// oneOfData_case() guard (XStore_IO.cpp) -- same "1..12 is numeric" rule the
// aggregate kernels already use, so a bool/string/bytes column is a hole
// here too, not a crash or a silently-wrong 0.
inline std::vector<ZoneMapObservation> XStore::BuildZoneMapObservations(XStore &store, const XStore_Proto::RowData &row, uint32_t tracked_column_count) {
    const int tracked = static_cast<int>(tracked_column_count);
    std::vector<ZoneMapObservation> observations;
    observations.reserve(static_cast<size_t>(tracked));
    for (int col = 0; col < tracked; ++col) {
        bool is_numeric = false;
        ValueStruct vs{};
        if (col < row.fielddata_size()) {
            const XStore_Proto::FData &f_data = row.fielddata(col);
            is_numeric = f_data.oneOfData_case() > 0 && f_data.oneOfData_case() <= 12;
            if (is_numeric) {
                store.GetFDataValue(&f_data, &vs);
            }
        }
        observations.push_back({static_cast<uint32_t>(col), static_cast<double>(vs.value), is_numeric});
    }
    return observations;
}

#endif //XSTORE_XSTORE_INTERNAL_H
