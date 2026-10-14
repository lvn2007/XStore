#pragma once
#include <cstdint>
#include <cstring>
#include <string>

#include "XStore.pb.h"

// Canonical byte encoding for an FData value, used identically at insert
// time (BuildCatIdxObservations) and query time (FindByValue) so a bloom
// bit set for a value at insert is found by the same value's lookup at
// query time. Distinguishes oneof case explicitly (int64Data=5 and
// doubleData=5.0 encode differently) -- this is exact-type equality, not
// GetFDataValue's aggregate-style numeric coercion into one double.
inline std::string EncodeFDataForCatIdx(const XStore_Proto::FData &f_data) {
    std::string out;
    auto case_id = f_data.oneOfData_case();
    out.push_back(static_cast<char>(static_cast<uint8_t>(case_id)));

    switch (case_id) {
        case XStore_Proto::FData::kDoubleData: {
            double v = f_data.doubledata();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kFloatData: {
            float v = f_data.floatdata();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kInt32Data: {
            int32_t v = f_data.int32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kInt64Data: {
            int64_t v = f_data.int64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kUint32Data: {
            uint32_t v = f_data.uint32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kUint64Data: {
            uint64_t v = f_data.uint64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kSint32Data: {
            int32_t v = f_data.sint32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kSint64Data: {
            int64_t v = f_data.sint64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kFixed32Data: {
            uint32_t v = f_data.fixed32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kFixed64Data: {
            uint64_t v = f_data.fixed64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kSfixed32Data: {
            int32_t v = f_data.sfixed32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kSfixed64Data: {
            int64_t v = f_data.sfixed64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kBoolData: {
            char v = f_data.booldata() ? 1 : 0;
            out.push_back(v);
            break;
        }
        case XStore_Proto::FData::kStringData:
            out.append(f_data.stringdata());
            break;
        case XStore_Proto::FData::kBytesData:
            out.append(f_data.bytesdata());
            break;
        default:
            // ONEOFDATA_NOT_SET -- a hole. Caller (BuildCatIdxObservations)
            // is responsible for not emitting an observation for holes at
            // all; this function is never called on an unset FData in
            // practice, but returns the tag-only bytes rather than
            // asserting, matching the codebase's existing "hole is not an
            // error" precedent (see BuildZoneMapObservations).
            break;
    }
    return out;
}

#include <array>
#include <atomic>
#include <map>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "XStore/XStore_Crc32c.h"
#include "XStore/XStore_Tunables.h"
#include "XStore/XStore_ZoneMap.h" // reuse RowsPerBlock/BlockIndexForRow -- same block geometry as .zmap

// One tracked-column value observed for one row, ready to fold into that
// row's block's bloom entry. is_present=false means a hole/unset field --
// contributes nothing (mirrors ZoneMapObservation's is_numeric=false case).
struct CatIdxObservation {
    uint32_t    column_idx;
    std::string encoded_value; // from EncodeFDataForCatIdx -- empty/ignored if !is_present
    bool        is_present;
};

struct CatIdxFileHeader {
    uint32_t magic                = 0x43415449; // 'CATI'
    uint32_t version               = XStoreTunables::CATIDX_VERSION;
    uint32_t tracked_column_count  = 0;
    uint32_t block_count           = 0;
    uint64_t bitmap_section_offset = 0; // 0 = not compacted yet
    // 1 = may hold writes not yet synced alongside the data file (set before
    // the first write of a session, cleared only after data + index are
    // fsynced); 1 at startup => rebuild. See Durability A plan.
    uint32_t dirty    = 0;
    uint32_t reserved = 0;
};
static_assert(sizeof(CatIdxFileHeader) == 32);

// FNV-1a 64-bit -- trivial, no external dependency, used as the second of
// two hash functions for Kirsch-Mitzenmacher double-hashing
// (g_i(x) = h1(x) + i*h2(x)), h1 being the existing hardware-accelerated
// Crc32c(). Repurposing Crc32c outside its original WAL-framing-checksum
// scope is fine functionally (it's just a fast, well-distributed hash here).
inline uint64_t Fnv1a64(const void *data, size_t len) {
    uint64_t hash = 0xcbf29ce484222325ULL;
    const auto *bytes = static_cast<const unsigned char*>(data);
    for (size_t i = 0; i < len; ++i) {
        hash ^= bytes[i];
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

// Rows for CatIdxFile::RebuildFoldRows in a flat layout a caller reuses across chunks: row i is row_index[i], with
// values [value_end[i-1], value_end[i]) (value_end[-1] = 0). A value is kept as the two bloom hashes of its
// EncodeFDataForCatIdx bytes (computed by Add, outside the file mutex), the same hashes UpdateRow computes, so the fold
// itself only sets bits. A row with no values still counts: its block is written, as UpdateRow writes it.
struct CatIdxFoldBatch {
    struct Value {
        uint64_t h1;
        uint64_t h2;
        uint32_t column_idx;
    };
    std::vector<uint64_t> row_index;
    std::vector<size_t> value_end;
    std::vector<Value> values;
    void Clear() { row_index.clear(); value_end.clear(); values.clear(); }
    void Add(uint32_t column_idx, const std::string &encoded_value) {
        values.push_back({Crc32c(encoded_value.data(), encoded_value.size()),
                          Fnv1a64(encoded_value.data(), encoded_value.size()), column_idx});
    }
    // Closes the row whose values were added since the previous EndRow.
    void EndRow(uint64_t row) { row_index.push_back(row); value_end.push_back(values.size()); }
};

class CatIdxFile {
public:
    [[nodiscard]] bool Open(const std::string &path, uint16_t row_length,
                             uint32_t tracked_column_count = XStoreTunables::CATIDX_MAX_TRACKED_COLUMNS);

    void UpdateRow(uint64_t row_index, const std::vector<CatIdxObservation> &observations);

    // True (scan the block) also for a column past this file's width and for a block a running rebuild has not
    // finished.
    bool MightContain(uint64_t block_index, uint32_t column_idx, const std::string &encoded_value);

    // Live widening, stage 1 mechanics: same frontier protocol as ZoneMapFile (see its kNoRebuild comment). Rows at or
    // above F are skipped by UpdateRow and folded later by the builder through RebuildFoldRow; MightContain answers
    // "maybe" for a block not complete below F.
    static constexpr uint64_t kNoRebuild = UINT64_MAX;

    // Empties this file in place (same object and fd) at width new_tracked_column_count and starts a rebuild at
    // F = 0: old header marked dirty + fdatasync (on failure: returns false, nothing changed), ftruncate to 0, fresh
    // header (new width, dirty=1, no blocks, no bitmap section) + fdatasync, cache dropped. A crash at any point leaves
    // a file the next start rebuilds (dirty, or too short to hold a header). False also if the truncate fails.
    [[nodiscard]] bool BeginRebuild(uint32_t new_tracked_column_count);
    // Live builder only: BeginRebuild, unless a rebuild already runs on this file -- then kAlreadyRebuilding with nothing
    // touched. The check and the begin are one step under the file mutex, so two builders on one file cannot both begin
    // (the second would truncate the first's work while the first keeps advancing F).
    [[nodiscard]] BeginRebuildResult BeginLiveRebuild(uint32_t new_tracked_column_count);

    // Same contract as ZoneMapFile::EnterUntrustedRebuildState: F = 0 without writing anything; MightContain answers
    // "maybe" for every block for the rest of the session.
    void EnterUntrustedRebuildState();

    // Builder only, for a row at or above F while it holds the data-file READ range lock on it. Ignored for a row
    // below F or when no rebuild runs.
    void RebuildFoldRow(uint64_t row_index, const std::vector<CatIdxObservation> &observations);

    // Builder only: RebuildFoldRow for every row of the batch, in order, under ONE hold of the mutex (rows below F
    // skipped as RebuildFoldRow skips them). Each block is written once per run of consecutive rows in it, not once per
    // row; the bytes on disk are identical to folding the rows one by one. Returns false if a block or header write
    // failed (the caller must then not end the rebuild). No-op (true) when no rebuild runs.
    [[nodiscard]] bool RebuildFoldRows(const CatIdxFoldBatch &batch);

    // Builder only, while still holding the READ range lock over the rows just folded. Never moves F back.
    void AdvanceFrontier(uint64_t new_frontier);

    // F = kNoRebuild once the dirty header is durable; otherwise false with nothing changed (see
    // ZoneMapFile::FinishRebuild). The file stays dirty until a later MarkClean.
    [[nodiscard]] bool FinishRebuild();

    // Lock-free (the registry calls it under its own mutex, which must never wait on this file's mutex).
    [[nodiscard]] bool Rebuilding() const { return rebuilding_.load(std::memory_order_acquire); }

    // Test-visible: the rebuild frontier F (kNoRebuild when no rebuild runs).
    [[nodiscard]] uint64_t FrontierForTests() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return frontier_;
    }

    // Registry pin, same contract as ZoneMapFile::Pin (taken only by XStore::PinCatIdx under the registry mutex).
    class Pin {
    public:
        Pin() = default;
        Pin(Pin &&other) noexcept : file_(other.file_) { other.file_ = nullptr; }
        Pin &operator=(Pin &&other) noexcept {
            if (this != &other) { Release(); file_ = other.file_; other.file_ = nullptr; }
            return *this;
        }
        Pin(const Pin &) = delete;
        Pin &operator=(const Pin &) = delete;
        ~Pin() { Release(); }
        CatIdxFile &operator*() const { return *file_; }
        CatIdxFile *operator->() const { return file_; }
        CatIdxFile *get() const { return file_; }
    private:
        // Only XStore::Pin* create a pin, under the registry mutex (eviction and retire read the count under it).
        friend class XStore;
        explicit Pin(CatIdxFile *file) : file_(file) { if (file_ != nullptr) file_->pins_.fetch_add(1, std::memory_order_relaxed); }
        void Release() { if (file_ != nullptr) file_->pins_.fetch_sub(1, std::memory_order_release); file_ = nullptr; }
        CatIdxFile *file_ = nullptr;
    };

    [[nodiscard]] bool Evictable() const {
        return pins_.load(std::memory_order_acquire) == 0 && !rebuilding_.load(std::memory_order_acquire);
    }

    // A live Pin exists. Pins are taken only under the registry mutex, so a caller holding it that sees false knows
    // nobody holds the file and nobody can start to.
    [[nodiscard]] bool Pinned() const { return pins_.load(std::memory_order_acquire) != 0; }

    // Appends ALL surviving columns' value->row-indices directories to the
    // bitmap section in one call (commit-safety: writes every column's
    // directory bytes and fsyncs ONCE BEFORE updating+fsyncing
    // header_.bitmap_section_offset ONCE -- a crash between the two leaves
    // harmless unreferenced bytes past the last known-good offset, same
    // "uncommitted tail is safely ignorable" shape the WAL itself relies
    // on). Deliberately one call per compaction pass, not one per column:
    // calling this once per column used to mean only the FIRST column's
    // write got crash-safe commit semantics -- header_.bitmap_section_offset
    // became non-zero after that first call, so a crash during a LATER
    // column's write left torn bytes INSIDE the already-committed,
    // scannable range. Row indices per value need not be pre-sorted --
    // stored as given (compaction naturally produces them in scan order).
    // Refused (nothing written) while a rebuild runs: the section would sit
    // where the builder's bloom blocks are about to go.
    void AppendBitmapSection(const std::unordered_map<uint32_t, std::map<std::string, std::vector<uint64_t>>> &columns);

    // False while a rebuild runs (also the untrusted state of EnterUntrustedRebuildState, which keeps an old section
    // on disk): the directory is a snapshot of rows the rebuild is re-deriving, and rows written since are not in it.
    bool HasBitmapSection() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return header_.bitmap_section_offset != 0 && frontier_ == kNoRebuild;
    }

    // Returns the exact row indices for (column_idx, encoded_value) if a
    // directory exists for that column AND the value is present in it.
    // nullopt covers BOTH "no directory for this column" (over cardinality
    // cap, or year not compacted yet) and "directory exists but value
    // absent" -- both mean the same thing to the caller: fall back to
    // bloom+real-scan.
    std::optional<std::vector<uint64_t>> LookupExact(uint32_t column_idx, const std::string &encoded_value);

    ~CatIdxFile();

    uint64_t RowsPerBlockValue() const { return rows_per_block_; }

    [[nodiscard]] uint32_t TrackedColumnCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return header_.tracked_column_count;
    }

    // Clears the on-disk dirty flag: fdatasync(index content), then header
    // dirty=0 + fdatasync. Returns false on any failure, leaving the file
    // marked dirty, and while a rebuild runs. The caller must have fdatasync'd
    // the .XSTORE data file FIRST -- this only makes the index itself durable.
    [[nodiscard]] bool MarkClean();

    // Marks the index dirty on disk now. A rebuild calls this before it touches
    // anything, so an early exit that folded no row cannot leave an empty index
    // marked clean. A failure is logged; the next UpdateRow retries.
    void MarkDirty();

    // Number of successful header dirty-mark writes (test-visible).
    [[nodiscard]] uint64_t DirtyMarkWritesForTests() const;

private:
    // Every method that writes index content calls this first (mutex_ held):
    // one predictable branch once the flag is on disk.
    void EnsureDirtyOnDisk() { if (!dirty_on_disk_) MarkDirtyOnDisk(); }
    bool BeginRebuildLocked(uint32_t new_tracked_column_count); // mutex_ held
    void MarkDirtyOnDisk();

    using BloomEntry = std::array<unsigned char, XStoreTunables::CATIDX_BLOOM_BYTES_PER_ENTRY>;

    void EnsureBlockLoaded(uint64_t block_index);
    void EvictIfOverCapacity();
    off_t BloomSectionFileOffset(uint64_t block_index) const;
    bool WriteFreshHeader(uint16_t row_length, uint32_t tracked_column_count);
    // False if the bloom block or the block_count header write failed.
    bool FlushBlock(uint64_t block_index);
    void UpdateRowLocked(uint64_t row_index, const std::vector<CatIdxObservation> &observations);

    int fd_ = -1;
    CatIdxFileHeader header_;
    uint64_t rows_per_block_ = 0;
    bool dirty_on_disk_ = false; // header_.dirty == 1 is durable on disk
    uint64_t dirty_mark_writes_ = 0;
    uint64_t frontier_ = kNoRebuild; // rebuild frontier F, see kNoRebuild
    std::atomic<bool> rebuilding_{false}; // frontier_ != kNoRebuild; written under mutex_, read without it
    std::atomic<uint32_t> pins_{0};       // live Pin objects
    std::unordered_map<uint64_t, std::vector<BloomEntry>> cache_; // block_index -> per-column bloom bytes
    std::vector<uint64_t> lru_order_;
    mutable std::mutex mutex_;
};
