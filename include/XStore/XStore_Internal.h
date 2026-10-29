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

// Definition of XStore::ZoneMapObservationOf (declared in XStore.h,
// private static). Numeric-case check mirrors ProcessFileRangeSelect/
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

inline void XStore::WalRecord::ReserveRows(size_t rows, size_t widest_fields) {
    const size_t columns = std::min<size_t>(tracked_column_count, widest_fields);
    wire.reserve(rows * RowStride());
    zmap_values.reserve(rows * columns);
    zmap_numeric.reserve(rows * columns);
    catidx_vals.reserve(rows * columns);
    obs_end.reserve(rows);
}

// Definition of XStore::WalRecord::AppendRow (declared in XStore.h). The
// observations are built once at WAL enqueue time (the row's wire bytes --
// already serialized -- are all ApplyWalRecord has later, not the original
// RowData, so this can't be deferred to apply time). Both indexes see only
// columns actually present in the row (col < row.fielddata_size()), clamped
// to tracked_column_count:
// - zone map: one observation per present column. Columns past that, up to
//   tracked_column_count, are exactly as much a hole as an explicitly unset
//   field at a present index (XStoreZoneMapTest.UnaryInsertTreatsHoleAsNullNotZero
//   -- an entirely empty row still records a hole at column 0), but
//   zone_map_pad_to bumps null_count for those instead of this building an
//   explicit is_numeric=false entry per hole -- same total effect.
// - catidx: ALL FData oneof cases (no numeric gate: the bloom works on
//   AppendFDataForCatIdx's bytes regardless of type), but only columns that
//   are also SET -- CatIdxFile always skipped unset ones, so omitting them is
//   the same effect as passing them as not present. Each value is hashed here
//   into its bloom bit positions (BloomValueOfEncoded), off the writer thread.
// tracked_column_count is max(EffectiveTrackedColumnCount(cfg_struct), the
// request's widest accepted row) at every call site.
inline void XStore::WalRecord::AppendRow(XStore &store, uint64_t epoch, const XStore_Proto::RowData &data) {
    const size_t at = wire.size();
    wire.resize(at + RowStride());   // the zero fill pads the tail past the serialized size
    std::memcpy(&wire[at], &epoch, sizeof(epoch));
    // Reuses the sizes the caller's ByteSizeLong() cached (SerializeToArray would recompute them), as SendProto in
    // ZMQ_Server.cpp.
    (void)data.SerializeWithCachedSizesToArray(reinterpret_cast<uint8_t *>(&wire[at + sizeof(uint64_t)]));
    // col < fielddata_size, and a column takes at least 2 of the row's u16 row_length bytes: it fits column_idx.
    const int tracked = std::min(static_cast<int>(tracked_column_count), data.fielddata_size());
    CatIdxEncodeBuffer encoded;   // inline storage: no allocation, no out-of-line append per value
    for (int col = 0; col < tracked; ++col) {
        const XStore_Proto::FData &f_data = data.fielddata(col);
        const ZoneMapObservation obs = ZoneMapObservationOf(store, f_data, static_cast<uint32_t>(col));
        AddZoneMapObs(obs.value, obs.is_numeric);
        if (f_data.oneOfData_case() != XStore_Proto::FData::ONEOFDATA_NOT_SET) {
            encoded.clear();
            AppendFDataForCatIdx(encoded, f_data);
            catidx_vals.push_back(BloomValueOfEncoded(static_cast<uint32_t>(col), encoded.data(), encoded.size()));
        }
    }
    obs_end.push_back({zmap_values.size(), catidx_vals.size()});
    ++row_count;
}

// The rows of an insert request as MultiRowInsertRows consumes them. ParsedRows wraps the repeated field of a request that was
// parsed into an object tree (the normal path); WireRows (XStore_BatchOps.cpp) reads them from the received bytes instead.
// AppendTo returns false only for a row that cannot be read (never for ParsedRows).
template <typename Data>
class ParsedRows {
public:
    explicit ParsedRows(google::protobuf::RepeatedPtrField<Data> &rows) : rows_(rows) {}
    int Size() const { return rows_.size(); }
    uint64_t Timestamp(int i) const { return rows_.Get(i).timestamp(); }
    int FieldCount(int i) const { return rows_.Get(i).data().fielddata_size(); }
    size_t RowBytes(int i) const { return rows_.Get(i).data().ByteSizeLong(); }   // also caches the sizes AppendTo serializes with
    // STABLE on purpose: for duplicate timestamps the last occurrence in the request must win (it is applied last, as a series of
    // UnaryInserts would do). Sorts the field's element pointers (no message copies); serial, because without TBB par_unseq is
    // serial too. Physically reorders the caller's rows in place: an index array instead was benchmarked SLOWER (every per-row
    // lookup became a random access into the pointer array, losing hardware prefetch).
    void SortByTimestamp() {
        std::stable_sort(rows_.pointer_begin(), rows_.pointer_end(), [](const Data *a, const Data *b) { return a->timestamp() < b->timestamp(); });
    }
    template <typename Record>
    bool AppendTo(XStore &store, Record &record, int i) {
        const Data &entry = rows_.Get(i);
        record.AppendRow(store, entry.timestamp(), entry.data());
        return true;
    }

private:
    google::protobuf::RepeatedPtrField<Data> &rows_;
};

// Definition of XStore::MultiRowInsert (declared in XStore.h, private): the
// one body behind BatchInsert and RangeInsert. op is the log/status tag
// ("BATCH INSERT" / "RANGE INSERT"); InsertRows resolves the request's rows
// field by overload, so each instantiation is the same code the two
// hand-written copies were.
template <typename Req, typename Rep>
void XStore::MultiRowInsert(Req *proto_from_client, Rep *return_data, const char *op, xstore::MemoryPool::Clock::time_point deadline) {
    ParsedRows<typename std::remove_reference_t<decltype(InsertRows(proto_from_client))>::value_type> rows(InsertRows(proto_from_client));
    // ByteSizeLong() is cached from the parse (or recomputed cheaply): the closest thing to "the received message's
    // size" available once a request has already gone through the object-tree parse, for the "message bytes" term
    // of the memory estimate below.
    (void)MultiRowInsertRows(rows, proto_from_client->dbname(), proto_from_client->durabilitymode(), return_data, op, deadline,
                              proto_from_client->ByteSizeLong());
}

// The body of MultiRowInsert over any source of rows (ParsedRows above, or WireRows for a request still in its received bytes).
// Every WalRecord is built BEFORE the index width is grown and before any record is enqueued: a request whose rows cannot all
// be read (AppendTo false, WireRows only) has then had no effect at all and returns false, and the caller takes the parsed path,
// which fails such a request when it parses it, before any effect, exactly as it always did. true = handled (the reply is set).
template <typename Rows, typename Rep>
bool XStore::MultiRowInsertRows(Rows &rows, const std::string &requested_db_name, XStore_Proto::WalDurabilityMode durability_mode,
                                Rep *return_data, const char *op, xstore::MemoryPool::Clock::time_point deadline, size_t wire_bytes) {
    if (RejectInsertIfShutDown(return_data->mutable_insertstatus(), op)) return true;
    // Ensure DB Name string is all capitalized
    std::string db_name = requested_db_name;
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
        return true;
    }

    const int row_count = rows.Size();

    // Memory guard (owner decision 2026-10-28): reserve an estimate of this request's in-flight memory from the
    // process-wide pool BEFORE building anything -- a request the pool has no room for (or whose client has
    // already given up, see `deadline`) is rejected/abandoned here instead of running the server out of memory.
    // One pass over every row's field count and byte length also finds `widest` (the widest ACCEPTED row, used
    // further down to grow the index width) and `total_fields`, so it is not a second scan later.
    // Estimate: rows * (8 [timestamp] + row_length [stored bytes]) + total_fields * 25 (zone-map value 8 B + 1 bit +
    // CatIdxBloomValue 16 B per observed column) + wire_bytes (the received message, or its parsed-tree
    // equivalent) + rows * 64 (misc per-row vector/allocator overhead) -- a rough budget, not an exact accounting.
    // `wire_bytes` is already in memory (the OS's figure has it), so it is passed as already-held: it counts against the pool
    // limit but not again against the host's available memory. An ASYNC record co-owns `memory_lease` (set where the records
    // are enqueued below), so the reservation outlives this function until the WAL writer is done with the rows.
    int widest = 0;
    uint64_t total_fields = 0;
    for (int i = 0; i < row_count; i++) {
        const int fields = rows.FieldCount(i);
        total_fields += static_cast<uint64_t>(fields);
        if (fields > widest && rows.RowBytes(i) <= static_cast<size_t>(cfg_struct.rowlength())) {
            widest = fields;
        }
    }
    const uint64_t memory_estimate = static_cast<uint64_t>(row_count) * (8 + static_cast<uint64_t>(cfg_struct.rowlength()))
                                    + total_fields * 25 + static_cast<uint64_t>(wire_bytes) + static_cast<uint64_t>(row_count) * 64;
    std::shared_ptr<xstore::MemoryLease> memory_lease;
    const xstore::MemoryPool::Result pool_result = xstore::MemoryPool::Instance().Acquire(memory_estimate, deadline, memory_lease,
                                                                                               static_cast<uint64_t>(wire_bytes));
    if (pool_result != xstore::MemoryPool::Result::kGranted) {
        const char *why = pool_result == xstore::MemoryPool::Result::kTooLarge
                              ? "larger than the server's whole memory pool"
                              : "the client's deadline passed while waiting for free memory";
        std::string err_msg = fmt::format("[{}] [DB: {}] - rejected: {} (~{} bytes estimated, pool limit {} bytes)",
                                           op, db_name, why, memory_estimate, xstore::MemoryPool::Instance().Limit());
        SetTaggedStatus(return_data->mutable_insertstatus(), "[ERROR] ", err_msg);
        spdlog::warn(err_msg);
        return true;
    }

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
    auto bucket_ascending_rows = [&]() -> bool {
        bool have_window = false;
        unsigned short int curr_year = 0;
        int64_t window_last_epoch = 0;
        unsigned long long int prev_ts = 0;
        for (int i = 0; i < row_count; i++) {
            unsigned long long int ts = rows.Timestamp(i);
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
        // Stable (see ParsedRows::SortByTimestamp): the last occurrence of a duplicate timestamp wins; sorted input never
        // reaches this branch and keeps its order.
        rows.SortByTimestamp();
        data_map.clear();
        [[maybe_unused]] const bool ascending_now = bucket_ascending_rows();
    }
    // Invariant: from here rows are ascending, so every bucket's rows share
    // one year, so record.year (the bucket key, used to route the record to
    // its shard) == the year ApplyWalRecord derives from the first row's epoch.

    // durabilityMode now routes through the WAL -- see the field's comment
    // on BatchInsert_REQ / RangeInsert_REQ in XStore.proto.
    bool any_sync_failed = false;
    bool any_overloaded = false;   // a SYNC group failed because the WAL ring was full

    // `widest` (the widest ACCEPTED row of the whole request) was already found above, together with
    // `total_fields` for the memory estimate -- not recomputed here. The index width is grown to it
    // (MaybeGrowColumnCount below) BEFORE any WalRecord/zone-map file is created for it -- i.e. before the first
    // record is enqueued -- so a fresh DB's first batch is born at its true width instead of staying at the floor
    // until a restart picks up the grown column count.

    // Build the record of every year group first (no side effect yet), then grow the width and enqueue them.
    std::vector<WalRecord> records;
    records.reserve(data_map.size());
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
        record.zone_map_pad_to = record.tracked_column_count;
        // widest is the widest ACCEPTED row, so a true bound on every appended row's columns.
        record.ReserveRows(indices.size(), static_cast<size_t>(widest));

        for (unsigned long long int idx : indices) {
            size_t serialized_size = rows.RowBytes(static_cast<int>(idx));
            // Reject-not-truncate, same guarantee as UnaryInsert: a row
            // that doesn't fit its slot is dropped rather than silently
            // corrupting whatever follows it. Before AppendRow: a row once
            // appended cannot be taken back.
            if (serialized_size > static_cast<size_t>(cfg_struct.rowlength())) {
                continue;
            }
            if (!rows.AppendTo(*this, record, static_cast<int>(idx))) return false;   // WireRows only: nothing enqueued or grown yet
        }

        if (record.row_count == 0) {
            // Every row in this year-group was rejected as oversized --
            // nothing to write, not a failure of the write path itself.
            continue;
        }
        records.push_back(std::move(record));
    }

    // Every record's bytes (rows, zone-map/catidx observations) are now actually allocated, so the OS's own memory
    // figure already reflects them -- the lease stops being counted as "pending" (still charged against the
    // pool's own total, released in full when this function returns).
    memory_lease->MarkResident();

    MaybeGrowColumnCount(db_name, db_base_path_str, widest);

    for (WalRecord &record : records) {
        const unsigned short int year_key = record.year;   // the record is moved away below
        // BYPASS applies straight to the data file on this thread, same as
        // UnaryInsert's BYPASS path -- no WAL record ever written for it.
        // Everything else (SYNC/ASYNC) hands off to the WAL: EnqueueWalRecord
        // blocks until durable+applied for SYNC, returns immediately after
        // the lock-free enqueue for ASYNC.
        // An ASYNC record outlives this call (acknowledged before the writer has it), so it carries the memory lease until
        // the writer clears it; SYNC/BYPASS callers wait here, the handler-scope lease already covers them.
        if (durability_mode == XStore_Proto::ASYNC) record.memory_lease = memory_lease;
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
        return true;
    }
    std::string success_msg = fmt::format("[{}] [DB: {}] - Inserted {} elements successfully", op, db_name, row_count);
    SetTaggedStatus(return_data->mutable_insertstatus(), "[INFO] ", success_msg);
    spdlog::info(success_msg);
    return true;
}

#endif //XSTORE_XSTORE_INTERNAL_H
