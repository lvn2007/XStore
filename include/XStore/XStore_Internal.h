#ifndef XSTORE_XSTORE_INTERNAL_H
#define XSTORE_XSTORE_INTERNAL_H

#include "include/XStore/XStore.h"
#include "include/DataStructs/TimeStruct.h"
#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cstring>
#include <execution>
#include <iterator>
#include <string_view>
#include <type_traits>
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

inline void AppendToYearBucket(YearBucketMap &buckets, unsigned short int year, unsigned long long int idx,
                               size_t first_bucket_reserve = 0) {
    for (auto &bucket : buckets) {
        if (bucket.first == year) {
            bucket.second.push_back(idx);
            return;
        }
    }
    // Insert passes its row count: the usual single-year request then fills one allocation instead of ~log2(n) regrowths.
    if (first_bucket_reserve != 0 && buckets.empty()) {
        std::vector<unsigned long long int> first;
        first.reserve(first_bucket_reserve);
        first.push_back(idx);
        buckets.emplace_back(year, std::move(first));
        return;
    }
    buckets.emplace_back(year, std::vector<unsigned long long int>{idx});
}

// std::execution::par_unseq dispatches work across a thread pool -- worth it
// once a batch is big enough that sorting it in parallel outweighs that
// dispatch/join overhead, but for a small batch (the common case for most
// callers) the overhead alone can cost more than a plain sequential sort
// would take to run the whole thing. Used by BatchQuery/BatchDelete's
// "sort incoming timestamps first" step. NOT stable (equal timestamps may swap),
// so not used by Batch/RangeInsert: MultiRowInsert (below) only sorts on an
// inversion, with std::stable_sort, so the last duplicate wins.
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

// Definition of XStore::MultiRowInsert (declared in XStore.h, private): the
// one body behind BatchInsert and RangeInsert. op is the log/status tag
// ("BATCH INSERT" / "RANGE INSERT"); InsertRows resolves the request's rows
// field by overload, so each instantiation is the same code the two
// hand-written copies were.
template <typename Req, typename Rep>
void XStore::MultiRowInsert(Req *proto_from_client, Rep *return_data, const char *op) {
    using Data = typename std::remove_reference_t<decltype(InsertRows(proto_from_client))>::value_type;
    if (RejectInsertIfShutDown(return_data->mutable_insertstatus(), op)) return;
    // Ensure DB Name string is all capitalized
    std::string db_name = proto_from_client->dbname();
    ToUpperCase(db_name.begin(), db_name.end());

    // Base directory of a DB
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    // Fetch cfgStruct from cache, loading from disk on first use. This also
    // covers "DB does not exist": GetOrLoadCfg stats the .CFG file itself on
    // a cache miss, so a separate upfront is_directory() check on every call
    // (cache hit or not) would just be a redundant stat -- dropped, same fix
    // already applied to Unary ops.
    const uint32_t cfg_epoch = CfgEpoch(); // before the cfg read (see WalRecord::cfg_epoch)
    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[{}] [DB: {}] - DB does not exist", op, db_name);
        SetTaggedStatus(return_data->mutable_insertstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    google::protobuf::RepeatedPtrField<Data> &rows = InsertRows(proto_from_client);

    // Incoming data is expected ascending, so the common case never sorts.
    // The bucketing loop below notices the first inversion (one compare per
    // row against a timestamp it already loads); only then are the request's
    // rows sorted in place and the bucketing redone from scratch.
    // Organize incoming data into year buckets -- flat vector + linear scan
    // (see AppendToYearBucket), not std::map: a single request realistically
    // touches only a handful of distinct years, and the response is just a
    // status string, so bucket iteration order doesn't need to be
    // chronological the way BatchQuery's response does.
    YearBucketMap data_map;

    // Granularity check -- year-bucket via TimeStruct's closed-form calendar
    // calc instead of gmtime_r. Two reasons: (1) gmtime_r always treated the
    // raw timestamp as whole seconds regardless of CfgStruct.timeresolution(),
    // silently mis-bucketing MILLISECOND/MICROSECOND DBs into the wrong
    // year's file -- TimeStruct is resolution-aware, same as every other use
    // of it in this class. (2) Rows are ascending (checked, see above), so
    // consecutive rows usually share the same year: reuse the last
    // TimeStruct's [FirstEpoch, LastEpoch] window via one comparison
    // instead of rerunning the full calendar decomposition on every row.
    // Returns false at the first row older than its predecessor.
    const int row_count = rows.size();
    auto bucket_ascending_rows = [&]() -> bool {
        bool have_window = false;
        unsigned short int curr_year = 0;
        int64_t window_last_epoch = 0;
        unsigned long long int prev_ts = 0;
        for (int i = 0; i < row_count; i++) {
            unsigned long long int ts = rows.Get(i).timestamp();
            if (ts < prev_ts) return false;   // cold: the C++17 build has no [[unlikely]]
            prev_ts = ts;
            if (!have_window || static_cast<int64_t>(ts) > window_last_epoch) {
                TimeStruct row_ts(static_cast<time_t>(ts), cfg_struct.timeresolution());
                curr_year = row_ts.year;
                window_last_epoch = row_ts.last_epoch;
                have_window = true;
            }
            AppendToYearBucket(data_map, curr_year, static_cast<unsigned long long int>(i), static_cast<size_t>(row_count));
        }
        return true;
    };
    if (!bucket_ascending_rows()) {
        // STABLE on purpose: for duplicate timestamps the last occurrence in the request must win (it is applied
        // last, as a series of UnaryInserts would do; sorted input never reaches this branch and keeps that order).
        // Sorts the field's element pointers (no message copies); serial, because without TBB par_unseq is serial too.
        // Physically reorders the caller's rows in place: an index array instead was benchmarked SLOWER (every
        // per-row lookup below became a random access into the pointer array, losing hardware prefetch).
        std::stable_sort(rows.pointer_begin(), rows.pointer_end(),
                         [](const Data *a, const Data *b) {
                             return a->timestamp() < b->timestamp();
                         });
        data_map.clear();
        [[maybe_unused]] const bool ascending_now = bucket_ascending_rows();
    }
    // Invariant: from here rows are ascending, so every bucket's rows share
    // one year, so record.year (the bucket key, used to route the record to
    // its shard) == the year ApplyWalRecord derives from rows.front().

    // durabilityMode now routes through the WAL -- see the field's comment
    // on BatchInsert_REQ / RangeInsert_REQ in XStore.proto.
    const XStore_Proto::WalDurabilityMode durability_mode = proto_from_client->durabilitymode();
    bool any_sync_failed = false;
    bool any_overloaded = false;   // a SYNC group failed because the WAL ring was full

    // Grow the index width to this request's widest row BEFORE any
    // WalRecord/zone-map file is created for it -- one pass over the whole
    // request, done once (not per year-bucket group, not per row), so a
    // fresh DB's first batch is born at its true width instead of staying
    // at the floor until a restart picks up the grown column count.
    int widest = 0;
    for (int i = 0; i < row_count; i++) {
        // Rejected oversized rows must not grow the persisted width.
        const auto &row_data = rows.Get(i).data();
        if (row_data.fielddata_size() > widest &&
            row_data.ByteSizeLong() <= static_cast<size_t>(cfg_struct.rowlength())) {
            widest = row_data.fielddata_size();
        }
    }
    MaybeGrowColumnCount(db_name, db_base_path_str, widest);

    // Iterate through each key (year) of dataMap
    for (const std::pair<unsigned short int, std::vector<unsigned long long int>> &pair : data_map) {
        unsigned short int year_key = pair.first;
        const std::vector<unsigned long long int> &indices = pair.second;

        // One WalRecord per year-bucket group. Rows within one group share
        // this call's cfg_struct, so db_name/db_base_path/time_resolution/
        // row_length/durability_mode are constant per record; only rows differ.
        WalRecord record;
        record.db_name = db_name;
        record.db_base_path = db_base_path_str;
        record.time_resolution = cfg_struct.timeresolution();
        record.row_length = cfg_struct.rowlength();
        record.cfg_epoch = cfg_epoch;
        record.year = year_key;  // routes the record to its (db, year) shard
        record.durability_mode = durability_mode;
        // One width for both the observations below and the zone map
        // ApplyWalRecord will open/create for them -- see
        // WalRecord::tracked_column_count. cfg_struct is this call's own
        // local snapshot, so this stays constant across the whole loop even
        // as MaybeGrowColumnCount moves the persisted value underneath.
        // Folded in with the pre-grown `widest` so a brand-new index file
        // is created at the request's true width on this very first write,
        // not just EffectiveTrackedColumnCount's pre-growth floor.
        record.tracked_column_count = std::max(EffectiveTrackedColumnCount(cfg_struct),
                                                std::min<uint32_t>(static_cast<uint32_t>(widest), XStoreTunables::MAX_TRACKED_COLUMN_COUNT));
        record.rows.reserve(indices.size());

        for (unsigned long long int idx : indices) {
            const Data &entry = rows.Get(static_cast<int>(idx));
            size_t serialized_size = entry.data().ByteSizeLong();
            // Reject-not-truncate, same guarantee as UnaryInsert: a row
            // that doesn't fit its slot is dropped rather than silently
            // corrupting whatever follows it.
            if (serialized_size > static_cast<size_t>(cfg_struct.rowlength())) {
                continue;
            }
            // The '\0' fill already zero-pads the tail past serialized_size (no memset needed).
            std::string row_bytes(cfg_struct.rowlength(), '\0');
            // Reuses the sizes ByteSizeLong() just cached (SerializeToArray would recompute them), as SendProto in ZMQ_Server.cpp.
            [[maybe_unused]] const uint8_t *serialize_end =
                entry.data().SerializeWithCachedSizesToArray(reinterpret_cast<uint8_t *>(row_bytes.data()));
            record.rows.push_back(WalRow{entry.timestamp(), std::move(row_bytes),
                                          BuildZoneMapObservations(*this, entry.data(), record.tracked_column_count),
                                          BuildCatIdxObservations(*this, entry.data(), record.tracked_column_count),
                                          record.tracked_column_count});
        }

        if (record.rows.empty()) {
            // Every row in this year-group was rejected as oversized --
            // nothing to write, not a failure of the write path itself.
            continue;
        }

        // BYPASS applies straight to the data file on this thread, same as
        // UnaryInsert's BYPASS path -- no WAL record ever written for it.
        // Everything else (SYNC/ASYNC) hands off to the WAL: EnqueueWalRecord
        // blocks until durable+applied for SYNC, returns immediately after
        // the lock-free enqueue for ASYNC.
        bool ok = (durability_mode == XStore_Proto::BYPASS)
            ? ApplyWalRecord(record)
            : EnqueueWalRecord(std::move(record), &any_overloaded);
        if (!ok) {
            any_sync_failed = true;
            spdlog::error("[{}] [DB: {}] - WAL write failed for {}.XSTORE", op, db_name, year_key);
        }
    }

    // Return insert status. any_sync_failed is only ever set on a genuine
    // ApplyWalRecord/EnqueueWalRecord failure above -- true durability
    // failures for SYNC/BYPASS, but also a real enqueue failure for ASYNC
    // (queue-full/backpressure timeout, allocation failure, etc.), which is
    // just as real a failure as the other modes' and must not be masked by
    // the unconditional success message below -- no mode condition here.
    if (any_sync_failed) {
        std::string err_msg = fmt::format("[{}] [DB: {}] - Inserted {} elements, but the write failed for at least one group",
                                           op, db_name, row_count);
        if (any_overloaded) err_msg += std::string("; ") + kWalOverloadedText;
        SetTaggedStatus(return_data->mutable_insertstatus(), "[ERROR] ", err_msg);
        spdlog::error(err_msg);
        return;
    }
    std::string success_msg = fmt::format("[{}] [DB: {}] - Inserted {} elements successfully", op, db_name, row_count);
    SetTaggedStatus(return_data->mutable_insertstatus(), "[INFO] ", success_msg);
    spdlog::info(success_msg);
}

#endif //XSTORE_XSTORE_INTERNAL_H
