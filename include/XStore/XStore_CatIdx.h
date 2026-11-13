#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

#include "XStore.pb.h"

// Canonical byte encoding for an FData value, used identically at insert
// time (WalRecord::AppendRowWithObs for a record the request thread indexes,
// FoldStoredRows for a rows-only one) and query time (FindByValue) so a bloom
// bit set for a value at insert is found by the same value's lookup at
// query time. Distinguishes oneof case explicitly (int64Data=5 and
// doubleData=5.0 encode differently) -- this is exact-type equality, not
// GetFDataValue's aggregate-style numeric coercion into one double.
// Appends the encoding to `out`: the insert path encodes into a reused
// scratch buffer and hashes it there; EncodeFDataForCatIdx below wraps this one
// implementation, so the two can never drift.
// A reusable encoding sink for the insert path: the tag and every scalar (<= 9 bytes) live in the inline array, so the
// per-column appends compile to a few stores instead of out-of-line std::string::_M_append calls. A longer encoding
// (string / bytes value) moves to a heap string and stays there until clear(). Interface = the three std::string calls
// AppendFDataForCatIdx makes.
class CatIdxEncodeBuffer {
public:
    static constexpr size_t kInlineBytes = 64;
    void clear() { size_ = 0; heap_.clear(); spilled_ = false; }
    [[nodiscard]] const char *data() const { return spilled_ ? heap_.data() : inline_; }
    [[nodiscard]] size_t size() const { return size_; }
    void push_back(char c) { append(&c, 1); }
    void append(const std::string &s) { append(s.data(), s.size()); }
    void append(const char *bytes, size_t len) {
        if (!spilled_ && size_ + len <= kInlineBytes) {
            std::memcpy(inline_ + size_, bytes, len);
            size_ += len;
            return;
        }
        Spill(bytes, len);
    }

private:
    void Spill(const char *bytes, size_t len) {
        if (!spilled_) {
            heap_.assign(inline_, size_);
            spilled_ = true;
        }
        heap_.append(bytes, len);
        size_ += len;
    }
    char inline_[kInlineBytes];
    size_t size_ = 0;
    bool spilled_ = false;
    std::string heap_;
};

// Out is std::string (EncodeFDataForCatIdx, the query side) or CatIdxEncodeBuffer (the insert path).
template <class Out>
inline void AppendFDataForCatIdx(Out &out, const XStore_Proto::FData &f_data) {
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
            // ONEOFDATA_NOT_SET -- a hole. Callers (WalRecord::AppendRowWithObs,
            // FoldStoredRows) are responsible for not emitting an observation for holes at
            // all; this function is never called on an unset FData in
            // practice, but appends the tag-only bytes rather than
            // asserting, matching the codebase's existing "hole is not an
            // error" precedent (see ZoneMapObservationOf).
            break;
    }
}

inline std::string EncodeFDataForCatIdx(const XStore_Proto::FData &f_data) {
    std::string out;
    AppendFDataForCatIdx(out, f_data);
    return out;
}

#include <array>
#include <atomic>
#include <map>
#include <memory>
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
    uint64_t bitmap_section_offset = 0; // 0 = not bitmapped yet
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

inline constexpr size_t kCatIdxBloomBits = XStoreTunables::CATIDX_BLOOM_BYTES_PER_ENTRY * 8;
static_assert(XStoreTunables::CATIDX_BLOOM_HASH_COUNT <= 8);
static_assert(kCatIdxBloomBits <= 65536, "a bit position must fit a uint16_t");
static_assert(XStoreTunables::CATIDX_MAX_TRACKED_COLUMNS <= 65535 && XStoreTunables::MAX_TRACKED_COLUMN_COUNT <= 65535,
              "a column index must fit a uint16_t");

// Bit i of a value's bloom entry: Kirsch-Mitzenmacher g_i = h1 + i*h2 (h1 = Crc32c, h2 = Fnv1a64 of the
// EncodeFDataForCatIdx bytes). The ONE position function: insert (every path) and MightContain use it, so the bits
// written and the bits checked cannot drift. kCatIdxBloomBits is constexpr, so the % compiles to a multiply.
inline constexpr size_t BloomBitPosition(uint64_t h1, uint64_t h2, size_t i) { return (h1 + i * h2) % kCatIdxBloomBits; }

class CatIdxBloomValue;
inline CatIdxBloomValue BloomValueOf(uint32_t column_idx, uint64_t h1, uint64_t h2);

// One value ready to fold: its column and its k bit positions inside the column's entry. The positions are in range
// (< kCatIdxBloomBits) by construction: BloomValueOf is the only writer, and a default value is column 0 with every
// position 0. So the fold needs no per-value check; the fold sets entry[bit >> 3] |=
// 1 << (bit & 7) and nothing else.
class CatIdxBloomValue {
public:
    CatIdxBloomValue() = default;
    [[nodiscard]] uint16_t ColumnIdx() const { return column_idx_; }
    [[nodiscard]] const uint16_t (&Bits() const)[XStoreTunables::CATIDX_BLOOM_HASH_COUNT] { return bit_; }

private:
    friend CatIdxBloomValue BloomValueOf(uint32_t column_idx, uint64_t h1, uint64_t h2);
    uint16_t column_idx_ = 0;
    uint16_t bit_[XStoreTunables::CATIDX_BLOOM_HASH_COUNT] = {};
};
static_assert(XStoreTunables::CATIDX_BLOOM_HASH_COUNT != 7 || sizeof(CatIdxBloomValue) == 16);
static_assert(std::is_trivially_copyable_v<CatIdxBloomValue>, "records are moved and copied in bulk");

// column_idx must be < 65536 (every caller passes a column below a file width, see the static_assert above).
inline CatIdxBloomValue BloomValueOf(uint32_t column_idx, uint64_t h1, uint64_t h2) {
    CatIdxBloomValue out;
    out.column_idx_ = static_cast<uint16_t>(column_idx);
    for (size_t i = 0; i < XStoreTunables::CATIDX_BLOOM_HASH_COUNT; ++i) {
        out.bit_[i] = static_cast<uint16_t>(BloomBitPosition(h1, h2, i));
    }
    return out;
}

inline CatIdxBloomValue BloomValueOfEncoded(uint32_t column_idx, const char *encoded_value, size_t len) {
    return BloomValueOf(column_idx, Crc32c(encoded_value, len), Fnv1a64(encoded_value, len));
}

// Rows for a batch fold (UpdateRows / RebuildFoldRows), non-owning: row i is row_index[i], with values
// [value_end[i-1], value_end[i]) (value_end[-1] = 0). A value whose column is >= column_limit is not folded (the writer
// passes the catidx width it read before its run loop; see ApplyWalRecord), nor one past the file's own width. A row with
// no values still counts: its block is written, as UpdateRow writes it. Everything pointed to must outlive the call.
struct CatIdxFoldView {
    const uint64_t *row_index = nullptr;
    const size_t *value_end = nullptr;
    size_t rows = 0;
    const CatIdxBloomValue *values = nullptr;
    uint32_t column_limit = UINT32_MAX;
};

// An owning CatIdxFoldView a caller reuses across chunks (the startup rebuild, the live widen). Add hashes the
// EncodeFDataForCatIdx bytes into bit positions (outside the file mutex), the same positions UpdateRow computes.
struct CatIdxFoldBatch {
    std::vector<uint64_t> row_index;
    std::vector<size_t> value_end;
    std::vector<CatIdxBloomValue> values;
    void Clear() { row_index.clear(); value_end.clear(); values.clear(); }
    void Add(uint32_t column_idx, const char *encoded_value, size_t len) {
        if (column_idx > UINT16_MAX) return; // no file is this wide (see the static_assert above): never folded anyway
        values.push_back(BloomValueOfEncoded(column_idx, encoded_value, len));
    }
    void Add(uint32_t column_idx, const std::string &encoded_value) { Add(column_idx, encoded_value.data(), encoded_value.size()); }
    // Closes the row whose values were added since the previous EndRow.
    void EndRow(uint64_t row) { row_index.push_back(row); value_end.push_back(values.size()); }
    [[nodiscard]] CatIdxFoldView View() const { return {row_index.data(), value_end.data(), row_index.size(), values.data()}; }
};

class CatIdxFile {
public:
    [[nodiscard]] bool Open(const std::string &path, uint16_t row_length,
                             uint32_t tracked_column_count = XStoreTunables::CATIDX_MAX_TRACKED_COLUMNS);

    void UpdateRow(uint64_t row_index, const std::vector<CatIdxObservation> &observations);

    // The writer's path (ApplyWalRecord, once per run): UpdateRow for every row of the batch, in order, under ONE hold
    // of the mutex, with UpdateRow's per-row rules (the old index of a keep-old rebuild gets every row, sharing the
    // batch's hashes). Each block is flushed once per run of consecutive rows in it, before this returns; the bytes on
    // disk are identical to UpdateRow row by row with the same values (a batch holds only present values), minus the
    // values at or past view.column_limit.
    void UpdateRows(const CatIdxFoldView &view);
    void UpdateRows(const CatIdxFoldBatch &batch) { UpdateRows(batch.View()); }

    // True (scan the block) also for a column past this file's width and for a block a running rebuild has not
    // finished, unless a keep-old rebuild still has the old index: that block is then answered by the old bloom (true
    // for a column past the old width).
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
    // Live builder only: kAlreadyRebuilding with nothing touched if a rebuild already runs on this file (check and begin
    // are one step under the file mutex). Same contract as ZoneMapFile::BeginLiveRebuild: the new file is created as "<path>.new" and
    // renamed over; the old file (its bitmap section too) serves until FinishRebuild and gets every UpdateRow.
    [[nodiscard]] BeginRebuildResult BeginLiveRebuild(uint32_t new_tracked_column_count);
    // Width of the old index a keep-old rebuild still serves; 0 when none. Lock-free.
    [[nodiscard]] uint32_t OldWidth() const { return old_width_.load(std::memory_order_acquire); }

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

    // Parallel live builder: same contract as ZoneMapFile::SetLiveFoldGroups / MarkFolded. UpdateRow then also
    // updates a row at or above F that lies in a folded group prefix.
    void SetLiveFoldGroups(uint64_t group_rows, uint64_t total_rows);
    uint64_t MarkFolded(uint64_t begin, uint64_t end);

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
    // on). Deliberately one call per bitmap build pass, not one per column:
    // calling this once per column used to mean only the FIRST column's
    // write got crash-safe commit semantics -- header_.bitmap_section_offset
    // became non-zero after that first call, so a crash during a LATER
    // column's write left torn bytes INSIDE the already-committed,
    // scannable range. Row indices per value need not be pre-sorted --
    // stored as given (bitmap build naturally produces them in scan order).
    // Refused (nothing written) while a rebuild runs: the section would sit
    // where the builder's bloom blocks are about to go.
    // Returns true when the section was committed. built_at_write_count: the WriteCount() the caller read before it began
    // scanning the rows the section was built from; the section is a snapshot of those rows, so it is refused (false, nothing
    // written) when a bloom block was written since -- that write is not in it and, with no section yet, invalidated nothing.
    // nullopt = the caller knows no write can have happened (tests).
    bool AppendBitmapSection(const std::unordered_map<uint32_t, std::map<std::string, std::vector<uint64_t>>> &columns,
                             std::optional<uint64_t> built_at_write_count = std::nullopt);

    // Test knobs for AppendBitmapSection's writes: every pwrite is cut to max_bytes_per_call (the kernel returns at most
    // 0x7ffff000 per call), and the write fails once fail_after_bytes have been written. SIZE_MAX, SIZE_MAX = no limit.
    static void SetBitmapWriteLimitsForTests(size_t max_bytes_per_call, size_t fail_after_bytes);

    // Number of bloom blocks written (FlushBlock) through this object since it was opened; in memory only, so a caller that
    // compares two readings must hold a Pin across them (the registry cannot then replace the object).
    [[nodiscard]] uint64_t WriteCount() {
        std::lock_guard<std::mutex> lock(mutex_);
        return write_count_;
    }

    // False while a rebuild runs (also the untrusted state of EnterUntrustedRebuildState, which keeps an old section
    // on disk): the directory is a snapshot of rows the rebuild is re-deriving, and rows written since are not in it.
    // A keep-old rebuild answers from the old index, whose section any write since bitmap build has invalidated.
    bool HasBitmapSection() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (frontier_ != kNoRebuild) return old_ != nullptr && old_->HasBitmapSection();
        return header_.bitmap_section_offset != 0;
    }

    // Returns the exact row indices for (column_idx, encoded_value) if a
    // directory exists for that column AND the value is present in it.
    // nullopt covers BOTH "no directory for this column" (over cardinality
    // cap, or year not bitmapped yet) and "directory exists but value
    // absent" -- both mean the same thing to the caller: fall back to
    // bloom+real-scan. Also nullopt while any row of the year is pending
    // (written, not folded yet): the directory does not hold it.
    std::optional<std::vector<uint64_t>> LookupExact(uint32_t column_idx, const std::string &encoded_value);

    ~CatIdxFile();

    // The rows of this year file whose index update is outstanding (nullptr = none are ever pending, e.g. a file
    // opened outside the registry). Set once, before the file is shared.
    void AttachPending(std::shared_ptr<xstore::PendingMap> pending) { pending_ = std::move(pending); }
    [[nodiscard]] const std::shared_ptr<xstore::PendingMap> &Pending() const { return pending_; }
    // Keep-old rebuild: the kept old index never has a map of its own (the outer object's is the year's).
    [[nodiscard]] bool OldHasPendingForTests() const { return old_ != nullptr && old_->pending_ != nullptr; }

    uint64_t RowsPerBlockValue() const { return rows_per_block_; }

    // Lock-free (a mirror of the header's width, stored under mutex_ wherever it changes): the writer reads it per
    // record while holding the record's range lock, and must not wait behind a builder's chunk-long mutex hold.
    [[nodiscard]] uint32_t TrackedColumnCount() const { return width_.load(std::memory_order_acquire); }

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
    std::shared_ptr<xstore::PendingMap> pending_;   // see AttachPending; read lock-free on the query path
    // Every method that writes index content calls this first (mutex_ held):
    // one predictable branch once the flag is on disk.
    void EnsureDirtyOnDisk() { if (!dirty_on_disk_) MarkDirtyOnDisk(); }
    bool BeginRebuildLocked(uint32_t new_tracked_column_count); // mutex_ held
    bool BeginKeepOldLocked(uint32_t new_tracked_column_count); // mutex_ held
    void AdoptOpenState(CatIdxFile &from) noexcept; // fresh object only, see ZoneMapFile::AdoptOpenState
    void MarkDirtyOnDisk();

    using BloomEntry = std::array<unsigned char, XStoreTunables::CATIDX_BLOOM_BYTES_PER_ENTRY>;

    void EnsureBlockLoaded(uint64_t block_index);
    void EvictIfOverCapacity();
    off_t BloomSectionFileOffset(uint64_t block_index) const;
    bool WriteFreshHeader(uint16_t row_length, uint32_t tracked_column_count);
    // False if the bloom block or the block_count header write failed.
    bool FlushBlock(uint64_t block_index);
    void UpdateRowLocked(uint64_t row_index, const std::vector<CatIdxObservation> &observations);
    // The batch fold shared by UpdateRows and RebuildFoldRows: every row with keep(row) true. mutex_ held.
    template <class Keep>
    [[nodiscard]] bool FoldBatchLocked(const CatIdxFoldView &view, Keep keep);

    int fd_ = -1;
    CatIdxFileHeader header_;
    uint64_t rows_per_block_ = 0;
    bool dirty_on_disk_ = false; // header_.dirty == 1 is durable on disk
    uint64_t dirty_mark_writes_ = 0;
    uint64_t frontier_ = kNoRebuild; // rebuild frontier F, see kNoRebuild
    RebuildWatermark fold_;          // folded group prefixes of a parallel live rebuild (inactive otherwise)
    std::atomic<bool> rebuilding_{false}; // frontier_ != kNoRebuild; written under mutex_, read without it
    std::atomic<uint32_t> pins_{0};       // live Pin objects
    std::unordered_map<uint64_t, std::vector<BloomEntry>> cache_; // block_index -> per-column bloom bytes
    std::vector<uint64_t> lru_order_;
    mutable std::mutex mutex_;
    uint64_t write_count_ = 0;   // FlushBlock calls, under mutex_
    // Appended last so every older member keeps its offset (hot-path layout unchanged by keep-old).
    std::string path_;                   // set by Open; the keep-old begin creates path_ + ".new"
    std::unique_ptr<CatIdxFile> old_;    // keep-old rebuild only: the old index, until FinishRebuild (under mutex_)
    std::atomic<uint32_t> old_width_{0}; // old_'s width, 0 without old_; written under mutex_, read without it
    std::atomic<uint32_t> width_{0};     // header_.tracked_column_count, see TrackedColumnCount
};
